setlocal EnableExtensions

REM ============================================================
REM run_paper_daily.bat  (RootA SSOT: E:\1_Data)
REM - Runs paper daily pipeline steps [1/9]..[9/9]
REM - Derives D from %ROOT%paper\fills.csv (latest BUY ymd else latest ymd)
REM - One-pass: orders(D) -> core -> ledger append
REM - Contract check: exec_date_unique must be D
REM - Post checks: paper_sync + pending_report JSON verdict
REM ============================================================

set "ROOT=%~dp0"
pushd "%ROOT%" || (
  echo [FAILED] pushd failed: "%ROOT%"
  exit /b 2
)

REM Python launcher (venv/absolute path first; fail-closed)
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
  echo         checked: "%ROOT%.venv\Scripts\python.exe"
  echo                  "E:\vibe\buffett\.venv\Scripts\python.exe"
  echo                  "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
  echo         install python or create venv and retry.
  goto :FAILED
)

echo [ROOT] %CD%
echo [PY] %PY%

REM ------------------------------------------------------------
REM [0/14] snapshot + hash the config used for this run (reproducibility)
REM + LOCK-B: require config sha256 to match approved_sha256 in paper_engine_config.lock.json
REM ------------------------------------------------------------
echo [0/14] snapshot+hash+lockcheck paper\paper_engine_config.json
set "CFG=%ROOT%paper\paper_engine_config.json"
set "LOCK=%ROOT%paper\paper_engine_config.lock.json"

if not exist "%CFG%" (
  echo [FAILED] config missing: %CFG%
  goto :FAILED
)

if not exist "%LOCK%" (
  echo [FAILED] lock missing: %LOCK%
  echo         run: %PY% tools\paper_engine_config_lock.py init
  goto :FAILED
)

REM timestamp for artifacts
set "CFG_TS="
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do set "CFG_TS=%%I"
if "%CFG_TS%"=="" (
  echo [FAILED] cannot get timestamp for config artifacts
  goto :FAILED
)

set "CFG_SNAP=%ROOT%2_Logs\paper_engine_config.used_%CFG_TS%.json"
set "CFG_HASHF=%ROOT%2_Logs\paper_engine_config.used_%CFG_TS%.sha256.txt"

REM snapshot (always)
copy /y "%CFG%" "%CFG_SNAP%" >nul
if errorlevel 1 (
  echo [FAILED] config snapshot copy failed: %CFG_SNAP%
  goto :FAILED
)

REM compute current sha
set "CFG_SHA="
for /f "usebackq delims=" %%H in (`
  %PY% -c "import hashlib,pathlib; p=pathlib.Path(r'%CFG%'); print(hashlib.sha256(p.read_bytes()).hexdigest())"
`) do set "CFG_SHA=%%H"

if "%CFG_SHA%"=="" (
  echo [FAILED] cannot compute sha256 for: %CFG%
  goto :FAILED
)

REM read approved sha from lock
set "APPROVED_SHA="
for /f "usebackq delims=" %%A in (`
  %PY% -c "import json,pathlib; p=pathlib.Path(r'%LOCK%'); j=json.loads(p.read_text(encoding='utf-8')); print(str(j.get('approved_sha256') or '').strip())"
`) do set "APPROVED_SHA=%%A"

if "%APPROVED_SHA%"=="" (
  echo [FAILED] approved_sha256 missing in lock: %LOCK%
  goto :FAILED
)

REM enforce match
if /I not "%CFG_SHA%"=="%APPROVED_SHA%" (
  echo [FAILED] CONFIG LOCK MISMATCH
  echo   current_sha256 = %CFG_SHA%
  echo   approved_sha256= %APPROVED_SHA%
  echo   hint: use %PY% tools\paper_engine_config_lock.py set ... to change config [updates lock]
  goto :FAILED
)

> "%CFG_HASHF%" echo %CFG_SHA%
echo [CONFIG_USED] ts=%CFG_TS% sha256=%CFG_SHA% snap=%CFG_SNAP%
echo [LOCK_OK] approved_sha256 matched

REM ------------------------------------------------------------
REM [1/9] update prices parquet
REM ------------------------------------------------------------
echo [1/9] paper_update_prices_parquet.py
%PY% paper_update_prices_parquet.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [2/9] sync candidates meta
REM ------------------------------------------------------------
echo [2/9] sync_candidates_meta.py
%PY% sync_candidates_meta.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [3/9] p0 daily check
REM ------------------------------------------------------------
echo [PRE] macro_signal_daily.py
%PY% tools\macro_signal_daily.py
if errorlevel 1 goto :FAILED

echo [3/9] p0_daily_check.py
%PY% p0_daily_check.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [4/9] gate daily
REM ------------------------------------------------------------
echo [4/9] gate_daily.py
%PY% gate_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [5/9] survivorship policy daily
REM ------------------------------------------------------------
echo [5/9] survivorship_policy_daily.py
%PY% survivorship_policy_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [6/9] liquidity filter daily
REM ------------------------------------------------------------
echo [6/9] liquidity_filter_daily.py
%PY% liquidity_filter_daily.py
if errorlevel 1 goto :FAILED


REM ------------------------------------------------------------
REM [6.25/9] candidates refresh (DART + KRX watchlist)
REM ------------------------------------------------------------
echo [6.25/9] generate_candidates_v41_1.py
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
if "%WATCH_CAUTION_LOOKBACK_DAYS%"=="" set "WATCH_CAUTION_LOOKBACK_DAYS=30"
if "%WATCH_SEARCH_LOOKBACK_DAYS%"=="" set "WATCH_SEARCH_LOOKBACK_DAYS=365"
echo [INFO] FUND_DART_REFRESH=%FUND_DART_REFRESH% FUND_KRX_WATCH_REFRESH=%FUND_KRX_WATCH_REFRESH%
%PY% generate_candidates_v41_1.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [6.5/9] sector score daily (snapshot + history append)
REM ------------------------------------------------------------
set "SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%"
if "%SIGNAL_FAIL_SOFT%"=="" set "SIGNAL_FAIL_SOFT=0"
echo [SIGNAL_POLICY] SIGNAL_FAIL_SOFT=%SIGNAL_FAIL_SOFT%
echo [6.5/9] tools\sector_score_daily.py
if "%KRX_API_KEY%"=="" (
  echo [INFO] sector_score_daily mode=MOCK (KRX_API_KEY empty)
) else (
  echo [INFO] sector_score_daily mode=REAL (KRX_API_KEY detected)
)
%PY% tools\sector_score_daily.py --krx-key "%KRX_API_KEY%"
if errorlevel 1 (
  echo [ERROR] sector_score_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
%PY% tools\check_signal_contract.py --stage sector --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] sector contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [6.55/9] collect Naver news into news_trading DB (default fail-closed)
REM ------------------------------------------------------------
echo [6.55/9] tools\news_collect_naver_daily.py
if "%NAVER_CLIENT_ID%"=="" (
  echo [INFO] NAVER_CLIENT_ID missing -> collector will skip fail-soft
) else (
  echo [INFO] NAVER_CLIENT_ID detected -> collector enabled
)
%PY% tools\news_collect_naver_daily.py
if errorlevel 1 (
  echo [ERROR] news_collect_naver_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

REM [6.6/9] news score daily (contract checked)
REM ------------------------------------------------------------
echo [6.6/9] tools\news_score_daily.py
%PY% tools\news_score_daily.py
if errorlevel 1 (
  echo [ERROR] news_score_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
%PY% tools\check_signal_contract.py --stage news --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] news contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)

REM ------------------------------------------------------------
REM [6.7/9] final score merge daily (contract checked)
REM ------------------------------------------------------------
echo [6.7/9] tools\final_score_merge_daily.py
%PY% tools\final_score_merge_daily.py
if errorlevel 1 (
  echo [ERROR] final_score_merge_daily failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
%PY% tools\check_signal_contract.py --stage final --root "%ROOT_NQ%"
if errorlevel 1 (
  echo [ERROR] final contract failed
  if "%SIGNAL_FAIL_SOFT%"=="1" (
    echo [WARN] SIGNAL_FAIL_SOFT=1 - continuing
  ) else (
    goto :FAILED
  )
)
REM ------------------------------------------------------------
REM [7/9] paper engine
REM ------------------------------------------------------------
echo [7/9] paper_engine.py
%PY% paper_engine.py >> %ROOT%2_Logs\run_paper_daily_last.txt 2>&1
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [8/9] audit daily
REM ------------------------------------------------------------
echo [8/9] audit_daily.py
%PY% audit_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [9/9] after close summary
REM ------------------------------------------------------------
echo [9/9] after_close_summary.cmd
call after_close_summary.cmd
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [10/14] derive D from paper\fills.csv max (YYYYMMDD)
REM - rule: latest BUY ymd first, else latest ymd
REM - NOTE: python -c inside .bat must escape % as %%
REM ------------------------------------------------------------
echo [10/14] derive D from paper\fills.csv max (YYYYMMDD)
if not exist "%ROOT%paper\fills.csv" (
  echo [FAILED] fills.csv missing: %ROOT%paper\fills.csv
  goto :FAILED
)

set "D_FILLS="
for /f "usebackq delims=" %%I in (`
  %PY% -c "import pandas as pd; df=pd.read_csv(r'%ROOT%paper\fills.csv',encoding='utf-8-sig'); ts=pd.to_datetime(df['datetime'],errors='coerce'); df['ymd']=ts.dt.strftime('%%Y%%m%%d'); d_any=(df['ymd'].dropna().max() if len(df) else ''); buy=df[df['side'].astype(str)=='BUY']; d_buy=(buy['ymd'].dropna().max() if len(buy) else ''); print(d_buy or d_any or '')"
`) do set "D_FILLS=%%I"

if "%D_FILLS%"=="" (
  echo [FAILED] cannot infer D from %ROOT%paper\fills.csv
  goto :FAILED
)

set "D=%D_FILLS%"
set "VIBE_EXEC_MODE=A"
set "D=%D%"

REM ------------------------------------------------------------
REM [11/14] onepass + ledger append (A-mode)
REM ------------------------------------------------------------
echo [11/14] p0_onepass_from_fills.py + ledger_append D=%D% (VIBE_EXEC_MODE=%VIBE_EXEC_MODE%)
%PY% tools\p0_onepass_from_fills.py %D%
if errorlevel 1 goto :FAILED

%PY% tools\ledger_append_from_orders_exec.py %D%
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12/14] contract check: exec_date_unique must be D
REM ------------------------------------------------------------
echo [12/14] contract exec_date==D for orders_%D%_exec.xlsx
%PY% -c "import pandas as pd,sys; D=r'%D%'; p=rf'%ROOT%paper\orders_{D}_exec.xlsx'; df=pd.read_excel(p); u=sorted(df['exec_date'].astype(str).unique().tolist()); print('exec_date_unique',u); sys.exit(0 if u==[D] else 2)"
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [12.5/14] optional broker dispatch/sync (default OFF)
REM BROKER_MODE: OFF | DRY | APPLY | APPLY_SYNC
REM ------------------------------------------------------------
if "%BROKER_MODE%"=="" set "BROKER_MODE=OFF"
if "%BROKER_VALIDATION_MODE%"=="" set "BROKER_VALIDATION_MODE=0"
if "%BROKER_BLOCK_PREFIXES%"=="" set "BROKER_BLOCK_PREFIXES=CAP_"
if "%BROKER_MIN_LIVE_ORDERS%"=="" set "BROKER_MIN_LIVE_ORDERS=3"
if "%BROKER_MOCK%"=="" set "BROKER_MOCK=auto"
if "%BROKER_CANCEL_OPEN%"=="" set "BROKER_CANCEL_OPEN=0"
if "%BROKER_CANCEL_MIN_AGE_MINUTES%"=="" set "BROKER_CANCEL_MIN_AGE_MINUTES=10"
if "%BROKER_CANCEL_MAX_ORDERS%"=="" set "BROKER_CANCEL_MAX_ORDERS=0"
if "%BROKER_PREFLIGHT_KIS%"=="" set "BROKER_PREFLIGHT_KIS=1"
if "%BROKER_PREFLIGHT_CODE%"=="" set "BROKER_PREFLIGHT_CODE=005930"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="" set "BROKER_PREFLIGHT_CHECK_BALANCE=1"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="" set "BROKER_PREFLIGHT_CHECK_OPEN_ORDERS=0"
if "%BROKER_PREFLIGHT_NOTIFY%"=="" set "BROKER_PREFLIGHT_NOTIFY=1"
if /I "%BROKER_VALIDATION_MODE%"=="1" if /I "%BROKER_MOCK%"=="auto" set "BROKER_MOCK=true"
set "BROKER_VALID_ARGS="
if /I "%BROKER_VALIDATION_MODE%"=="1" set "BROKER_VALID_ARGS=--validation-mode --allow-block-prefixes %BROKER_BLOCK_PREFIXES% --min-live-orders %BROKER_MIN_LIVE_ORDERS%"
set "BROKER_CANCEL_ARGS=--date %D% --mock %BROKER_MOCK% --min-age-minutes %BROKER_CANCEL_MIN_AGE_MINUTES% --max-cancels %BROKER_CANCEL_MAX_ORDERS%"
set "BROKER_PREFLIGHT_ARGS=--date %D% --mock %BROKER_MOCK% --code %BROKER_PREFLIGHT_CODE%"
if "%BROKER_PREFLIGHT_CHECK_BALANCE%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-balance"
if "%BROKER_PREFLIGHT_CHECK_OPEN_ORDERS%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --check-open-orders"
if "%BROKER_PREFLIGHT_NOTIFY%"=="1" set "BROKER_PREFLIGHT_ARGS=%BROKER_PREFLIGHT_ARGS% --notify-on-fail"
echo [12.5/14] broker mode=%BROKER_MODE% validation=%BROKER_VALIDATION_MODE% mock=%BROKER_MOCK% cancel_open=%BROKER_CANCEL_OPEN% preflight=%BROKER_PREFLIGHT_KIS%
if /I "%BROKER_MODE%"=="OFF" (
  echo [SKIP] broker dispatch BROKER_MODE=OFF
) else (
  if "%BROKER_PREFLIGHT_KIS%"=="1" (
    if /I "%BROKER_MODE%"=="DRY" (
      echo [BROKER] preflight skipped in DRY mode
    ) else (
      echo [BROKER] preflight KIS healthcheck
      %PY% tools\kis_healthcheck.py %BROKER_PREFLIGHT_ARGS%
      if errorlevel 1 (
        echo [FAILED] broker preflight failed. blocking order dispatch.
        goto :FAILED
      )
    )
  )
  if /I "%BROKER_MODE%"=="DRY" (
    %PY% tools\kis_order_dispatch_from_exec.py --date %D% --mock %BROKER_MOCK% %BROKER_VALID_ARGS%
    if errorlevel 1 goto :FAILED
    if "%BROKER_CANCEL_OPEN%"=="1" (
      %PY% tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS%
      if errorlevel 1 goto :FAILED
    )
  ) else (
    if /I "%BROKER_MODE%"=="APPLY" (
      %PY% tools\kis_order_dispatch_from_exec.py --date %D% --mock %BROKER_MOCK% %BROKER_VALID_ARGS% --apply
      if errorlevel 1 goto :FAILED
      if "%BROKER_CANCEL_OPEN%"=="1" (
        %PY% tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS% --apply
        if errorlevel 1 goto :FAILED
      )
    ) else (
      if /I "%BROKER_MODE%"=="APPLY_SYNC" (
        if "%BROKER_LIVE_PATH%"=="" set "BROKER_LIVE_PATH=E:\vibe\buffett\data\live\live_fills.csv"
        %PY% tools\kis_order_dispatch_from_exec.py --date %D% --mock %BROKER_MOCK% %BROKER_VALID_ARGS% --apply
        if errorlevel 1 goto :FAILED
        if "%BROKER_CANCEL_OPEN%"=="1" (
          %PY% tools\kis_cancel_open_orders.py %BROKER_CANCEL_ARGS% --apply
          if errorlevel 1 goto :FAILED
        )
        %PY% tools\kis_sync_fills_from_api.py --date %D% --bridge-write --bridge-live-path "%BROKER_LIVE_PATH%"
        if errorlevel 1 goto :FAILED
      ) else (
        echo [FAILED] invalid BROKER_MODE=%BROKER_MODE%
        goto :FAILED
      )
    )
  )
)
REM ------------------------------------------------------------
REM [13/14] paper_sync (fills_norm/trades_calc/pnl)
REM ------------------------------------------------------------
echo [13/14] paper_sync.py
%PY% paper_sync.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [14/16] signal integration daily (phase2/3/final as-of join)
REM ------------------------------------------------------------
echo [14/16] tools\signal_integration_daily.py
%PY% tools\signal_integration_daily.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [14.5/16] refresh backtest summary if due (freshness gate)
REM ------------------------------------------------------------
echo [14.5/16] report_if_due_v41_1.py
%PY% report_if_due_v41_1.py
if errorlevel 1 goto :FAILED

REM ------------------------------------------------------------
REM [15/16] live-vs-backtest feedback + auto optimize trigger
REM ------------------------------------------------------------
echo [15/16] live_vs_bt_paper_daily.py --date %D% --auto-optimize (aligned+gated)
%PY% live_vs_bt_paper_daily.py --date %D% --auto-optimize --align-window-trades 30 --min-shared-trades 10 --max-backtest-age-days 7 --min-oos-trades 20 --min-oos-pf 0.80 --min-stable-score -1000000000
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
  %PY% tools\indicator_diag_and_recommend.py --lookback-days 120 --min-universe 2000 --horizons 1,2,5
  if errorlevel 1 (
    echo [WARN] indicator_diag_and_recommend failed - continuing
  )
) else (
  echo [SKIP] indicator diag (INDICATOR_DIAG_AUTO=%INDICATOR_DIAG_AUTO%)
)

REM [16/16] pending report (JSON-based verdict; allow same-day pending)
echo [16/16] paper_pending_report.py + JSON verdict (pending==0)
%PY% tools\paper_pending_report.py
%PY% -c "import glob,json,os,sys; fs=glob.glob(r'%ROOT%2_Logs\paper_pending_report_*.json'); f=max(fs,key=os.path.getmtime); j=json.load(open(f,'r',encoding='utf-8')); pend=j.get('pending',[]) or []; act=j.get('active',[]) or []; px=str(j.get('prices_date_max') or ''); ok=(len(pend)==0) or all((str(r.get('entry_date') or '')==px and str(r.get('last_price_date_for_code') or '')==px) for r in pend); print('LATEST',f,'prices_date_max',px,'pending',len(pend),'active',len(act),'ok_same_day_pending',ok); sys.exit(0 if ok else 3)"
if errorlevel 1 goto :FAILED

echo.
REM [16.5/16] pending queue delta report (day-over-day)
echo [16.5/16] tools\pending_queue_delta_report.py
%PY% tools\pending_queue_delta_report.py
if errorlevel 1 (
  echo [WARN] pending_queue_delta_report failed - continuing
)

REM [16.6/16] kill-switch validation report (shadow score, fail-soft)
echo [16.6/16] tools\kill_switch_validation_report.py
%PY% tools\kill_switch_validation_report.py
if errorlevel 1 (
  echo [WARN] kill_switch_validation_report failed - continuing
)

echo [OK] finished
popd
endlocal & exit /b 0

:FAILED
echo.
echo [FAILED] step failed. ERRORLEVEL=%ERRORLEVEL%
popd
endlocal & exit /b 1


















