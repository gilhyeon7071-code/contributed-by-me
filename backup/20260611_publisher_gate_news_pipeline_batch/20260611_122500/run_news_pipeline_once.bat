@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || (
  echo [FAILED] pushd failed: "%ROOT%"
  exit /b 2
)

set "PY="
if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if not defined PY (
  echo [FAILED] python runtime not found
  exit /b 2
)

set "SESSION=%~1"
set "WINDOW_START=%~2"
set "WINDOW_END=%~3"
set "EXTRA_FLAG=%~4"
set "MODE_FILE=%ROOT%state\news_collect_mode.txt"
if exist "%MODE_FILE%" (
  set /p NEWS_COLLECT_MODE=<"%MODE_FILE%"
)
if "%NEWS_COLLECT_MODE%"=="" set "NEWS_COLLECT_MODE=production"

if "%SESSION%"=="" set "SESSION=daily"
if /I "%SESSION%"=="intraday" (
  if not defined NEWS_MAX_SYMBOLS set "NEWS_MAX_SYMBOLS=20"
  if not defined NEWS_DISPLAY set "NEWS_DISPLAY=10"
  if not defined NEWS_CACHE_TTL_SEC set "NEWS_CACHE_TTL_SEC=1800"
  if not defined NEWS_COLLECT_MAX_RUNTIME_SEC set "NEWS_COLLECT_MAX_RUNTIME_SEC=240"
  if not defined NEWS_OBSERVE_ONLY_SKIP set "NEWS_OBSERVE_ONLY_SKIP=1"
  if not defined NEWS_SOURCE_SIGNAL_SKIP set "NEWS_SOURCE_SIGNAL_SKIP=1"
)
if "%NEWS_OBSERVE_MAX_SYMBOLS%"=="" set "NEWS_OBSERVE_MAX_SYMBOLS=10"
if "%NEWS_GOOGLE_RSS_MAX_ITEMS%"=="" set "NEWS_GOOGLE_RSS_MAX_ITEMS=5"
set "LOCK_PARENT=%ROOT%2_Logs\locks"
set "LOCK_DIR=%LOCK_PARENT%\news_pipeline_once.lock"
if not exist "%LOCK_PARENT%" mkdir "%LOCK_PARENT%" >nul 2>nul
mkdir "%LOCK_DIR%" >nul 2>nul
if errorlevel 1 (
  echo [WARN] news pipeline already running; skip this run
  exit /b 0
)
echo %DATE% %TIME% session=%SESSION% > "%LOCK_DIR%\owner.txt"

echo [NEWS_PIPELINE] mode=%NEWS_COLLECT_MODE% session=%SESSION% window=%WINDOW_START%-%WINDOW_END%
echo [NEWS_PIPELINE] collect_max_runtime=%NEWS_COLLECT_MAX_RUNTIME_SEC% observe_skip=%NEWS_OBSERVE_ONLY_SKIP% source_signal_skip=%NEWS_SOURCE_SIGNAL_SKIP%
echo [NEWS_PIPELINE] python=%PY%
if not "%NEWS_COLLECT_SKIP%"=="1" (
  call :RUN_PY tools\news_collect_naver_daily.py --session-name "%SESSION%" --window-start "%WINDOW_START%" --window-end "%WINDOW_END%" %EXTRA_FLAG%
  if errorlevel 1 (
    echo [FAILED] news_collect_naver_daily.py
    call :RELEASE_LOCK
    exit /b 1
  )
)
if "%NEWS_COLLECT_SKIP%"=="1" (
  echo [NEWS_PIPELINE] NEWS_COLLECT_SKIP=1; skip news_collect_naver_daily.py
)

if not "%NEWS_SOURCE_SIGNAL_SKIP%"=="1" (
  call :RUN_PY tools\news_source_signal_daily.py
  if errorlevel 1 (
    echo [WARN] news_source_signal_daily.py failed; continuing with existing news scores
  )
)

if not "%NEWS_LLM_SKIP%"=="1" (
  call :RUN_PY tools\news_llm_score_daily.py
  if errorlevel 1 (
    echo [WARN] news_llm_score_daily.py failed; continuing with keyword/fallback scores
  )
)

if not "%NEWS_IMPLICATION_SKIP%"=="1" (
  call :RUN_PY tools\news_implication_daily.py
  if errorlevel 1 (
    echo [FAILED] news_implication_daily.py
    call :RELEASE_LOCK
    exit /b 1
  )
)

set "NEWS_SESSION_NAME=%SESSION%"
if /I "%SESSION%"=="intraday" (
  if "%NEWS_SCORE_REFERENCE_YMD%"=="" set "NEWS_SCORE_REFERENCE_YMD=today"
)
call :RUN_PY tools\news_score_daily.py
if errorlevel 1 (
  echo [FAILED] news_score_daily.py
  call :RELEASE_LOCK
  exit /b 1
)

if "%NEWS_OBSERVE_ONLY_SKIP%"=="1" (
  echo [NEWS_PIPELINE] NEWS_OBSERVE_ONLY_SKIP=1; skip observer reports
) else (
  call :RUN_OBSERVE_REPORTS
)

if not "%NEWS_PIPELINE_SKIP_FINAL%"=="1" (
  if not "%NEWS_CANDIDATES_SKIP%"=="1" (
    call :RUN_PY tools\news_candidates_daily.py
    if errorlevel 1 (
      echo [WARN] news_candidates_daily.py failed; continuing to final score merge
    )
  )
  call :RUN_PY tools\build_forward_estimate_snapshot.py --max-codes 80 --sleep 0.05 --http-retry-max 1 --http-backoff-base 0.2 --http-backoff-cap 1.5
  if errorlevel 1 (
    echo [FAILED] build_forward_estimate_snapshot.py
    call :RELEASE_LOCK
    exit /b 1
  )
  call :RUN_PY tools\final_score_merge_daily.py
  if errorlevel 1 (
    echo [FAILED] final_score_merge_daily.py
    call :RELEASE_LOCK
    exit /b 1
  )
  call :RUN_PY tools\build_news_signal_shadow_stage.py
  if errorlevel 1 (
    echo [WARN] build_news_signal_shadow_stage.py failed; continuing without shadow news signal stage
  )
)

if not "%NEWS_PIPELINE_SKIP_INTEGRATION%"=="1" (
  call :RUN_PY tools\signal_integration_daily.py
  if errorlevel 1 (
    echo [FAILED] signal_integration_daily.py
    call :RELEASE_LOCK
    exit /b 1
  )
)

echo [OK] news pipeline finished
call :RELEASE_LOCK
exit /b 0

:RELEASE_LOCK
if defined LOCK_DIR if exist "%LOCK_DIR%\owner.txt" del /f /q "%LOCK_DIR%\owner.txt" >nul 2>nul
if defined LOCK_DIR if exist "%LOCK_DIR%" rmdir "%LOCK_DIR%" >nul 2>nul
exit /b 0

:RUN_PY
set "PY_IS_EXE=0"
echo %PY% | findstr /I /R "\\.exe$" >nul && set "PY_IS_EXE=1"
if "%PY_IS_EXE%"=="1" (
  "%PY%" %*
) else (
  %PY% %*
)
exit /b %ERRORLEVEL%

:RUN_OBSERVE_REPORTS
call :RUN_PY tools\news_google_rss_probe.py --max-symbols %NEWS_OBSERVE_MAX_SYMBOLS% --max-items-per-symbol %NEWS_GOOGLE_RSS_MAX_ITEMS% --write-db
if errorlevel 1 (
  echo [WARN] news_google_rss_probe.py failed; continuing with existing news scores
)
call :RUN_PY tools\news_kis_title_probe.py --max-symbols %NEWS_OBSERVE_MAX_SYMBOLS% --max-pages 1 --write-db
if errorlevel 1 (
  echo [WARN] news_kis_title_probe.py failed; continuing with existing news scores
)
call :RUN_PY tools\news_google_rss_coverage_report.py
if errorlevel 1 (
  echo [WARN] news_google_rss_coverage_report.py failed; continuing with existing news scores
)
call :RUN_PY tools\news_source_score_impact_report.py
if errorlevel 1 (
  echo [WARN] news_source_score_impact_report.py failed; continuing with existing news scores
)
call :RUN_PY tools\news_source_match_quality_report.py
if errorlevel 1 (
  echo [WARN] news_source_match_quality_report.py failed; continuing with existing news scores
)
exit /b 0
