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

if "%SESSION%"=="" set "SESSION=daily"
if "%NEWS_OBSERVE_MAX_SYMBOLS%"=="" set "NEWS_OBSERVE_MAX_SYMBOLS=6"
if "%NEWS_GOOGLE_RSS_MAX_ITEMS%"=="" set "NEWS_GOOGLE_RSS_MAX_ITEMS=5"

echo [NEWS_PIPELINE] session=%SESSION% window=%WINDOW_START%-%WINDOW_END%
call :RUN_PY tools\news_collect_naver_daily.py --session-name "%SESSION%" --window-start "%WINDOW_START%" --window-end "%WINDOW_END%" %EXTRA_FLAG%
if errorlevel 1 (
  echo [FAILED] news_collect_naver_daily.py
  exit /b 1
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
    exit /b 1
  )
)

set "NEWS_SESSION_NAME=%SESSION%"
call :RUN_PY tools\news_score_daily.py
if errorlevel 1 (
  echo [FAILED] news_score_daily.py
  exit /b 1
)

if not "%NEWS_OBSERVE_ONLY_SKIP%"=="1" (
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
)

if not "%NEWS_PIPELINE_SKIP_FINAL%"=="1" (
  call :RUN_PY tools\build_forward_estimate_snapshot.py --max-codes 80 --sleep 0.05 --http-retry-max 1 --http-backoff-base 0.2 --http-backoff-cap 1.5
  if errorlevel 1 (
    echo [FAILED] build_forward_estimate_snapshot.py
    exit /b 1
  )
  call :RUN_PY tools\final_score_merge_daily.py
  if errorlevel 1 (
    echo [FAILED] final_score_merge_daily.py
    exit /b 1
  )
)

if not "%NEWS_PIPELINE_SKIP_INTEGRATION%"=="1" (
  call :RUN_PY tools\signal_integration_daily.py
  if errorlevel 1 (
    echo [FAILED] signal_integration_daily.py
    exit /b 1
  )
)

echo [OK] news pipeline finished
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
