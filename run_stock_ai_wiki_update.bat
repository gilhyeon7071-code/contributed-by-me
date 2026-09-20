@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || (
  echo [FAILED] pushd failed: "%ROOT%"
  exit /b 2
)

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [FAILED] python runtime not found
  popd
  exit /b 2
)

if "%STOCK_AI_WIKI_MAX_SYMBOLS%"=="" set "STOCK_AI_WIKI_MAX_SYMBOLS=10"
if "%STOCK_AI_WIKI_MAX_ITEMS%"=="" set "STOCK_AI_WIKI_MAX_ITEMS=5"
if "%STOCK_AI_WIKI_LIMIT%"=="" set "STOCK_AI_WIKI_LIMIT=10"
if "%STOCK_AI_WIKI_CACHE_TTL_SEC%"=="" set "STOCK_AI_WIKI_CACHE_TTL_SEC=600"
if "%STOCK_AI_WIKI_LOCK_STALE_MINUTES%"=="" set "STOCK_AI_WIKI_LOCK_STALE_MINUTES=90"
if "%STOCK_AI_WIKI_COVERAGE_TIMEOUT_SEC%"=="" set "STOCK_AI_WIKI_COVERAGE_TIMEOUT_SEC=600"

set "DATE_TMP=%ROOT%2_Logs\stock_ai_wiki_update_date.tmp"
"%PY%" -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('Asia/Seoul')).strftime('%%Y-%%m-%%d'))" > "%DATE_TMP%"
if errorlevel 1 (
  echo [FAILED] date resolution command failed
  popd
  exit /b 2
)
set /p STOCK_AI_WIKI_DATE=<"%DATE_TMP%"
if "%STOCK_AI_WIKI_DATE%"=="" (
  echo [FAILED] date resolution failed
  popd
  exit /b 2
)

set "LOCK_PARENT=%ROOT%2_Logs\locks"
set "LOCK_DIR=%LOCK_PARENT%\stock_ai_wiki_update.lock"
if not exist "%LOCK_PARENT%" mkdir "%LOCK_PARENT%" >nul 2>nul
if exist "%LOCK_DIR%" (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $lock=$env:LOCK_DIR; $owner=Join-Path $lock 'owner.txt'; $max=[double]$env:STOCK_AI_WIKI_LOCK_STALE_MINUTES; $stamp=(Get-Item -LiteralPath $owner -ErrorAction SilentlyContinue).LastWriteTime; if (-not $stamp) { $stamp=(Get-Item -LiteralPath $lock).LastWriteTime }; $age=((Get-Date)-$stamp).TotalMinutes; $cutoff=$stamp.AddMinutes(5); $active=Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_stock_ai_wiki_update\\.bat' -and $_.CreationDate -le $cutoff }; if ($age -ge $max -and -not $active) { Remove-Item -LiteralPath $lock -Recurse -Force; Write-Output ('[STOCK_AI_WIKI] stale lock cleared age_min={0:N1}' -f $age); exit 0 }; exit 1"
)
mkdir "%LOCK_DIR%" >nul 2>nul
if errorlevel 1 (
  echo [WARN] Stock-AI-Wiki update already running; skip this run
  popd
  exit /b 0
)
echo %DATE% %TIME% > "%LOCK_DIR%\owner.txt"

set "PROBE_JSON=%ROOT%2_Logs\google_news_rss_probe_latest.json"
set "COVERAGE_CSV=%ROOT%2_Logs\google_news_rss_coverage_report_latest.csv"
set "ARCHIVE_JSON=%ROOT%Stock-AI-Wiki\01_Sources\article_archive\google_rss_article_archive_seed_%STOCK_AI_WIKI_DATE%.json"

echo [STOCK_AI_WIKI] date=%STOCK_AI_WIKI_DATE%

call :RUN_PY tools\news_google_rss_probe.py --max-symbols %STOCK_AI_WIKI_MAX_SYMBOLS% --max-items-per-symbol %STOCK_AI_WIKI_MAX_ITEMS% --cache-ttl-sec %STOCK_AI_WIKI_CACHE_TTL_SEC%
if errorlevel 1 goto :FAIL

call :RUN_PY tools\run_step_with_timeout.py --name stock_ai_wiki_coverage_report --timeout-sec %STOCK_AI_WIKI_COVERAGE_TIMEOUT_SEC% --status-json 2_Logs\stock_ai_wiki_coverage_timeout_status_latest.json -- "%PY%" tools\news_google_rss_coverage_report.py
if errorlevel 1 goto :FAIL

call :RUN_PY Stock-AI-Wiki\tools\build_article_archive_seed.py --probe-json "%PROBE_JSON%" --out "%ARCHIVE_JSON%" --max-items-per-code 1 --apply
if errorlevel 1 goto :FAIL

REM Set the default OUTSIDE the if/else block (PLANS 2026-08-21 (41)).
REM Inside a parenthesized block cmd expands %STOCK_AI_WIKI_BODY_MAX_FETCH% at parse time,
REM i.e. before the set runs, so it became empty and produced "--max-fetch  --apply".
REM That made fetch_article_body fail with an argparse error on every run since 2026-06-29,
REM and the non-blocking [WARN] ... continuing hid it. Root cause of zero article bodies.
REM ASCII only: cmd reads .bat using the OEM codepage; non-ASCII here corrupts line parsing.
if "%STOCK_AI_WIKI_BODY_MAX_FETCH%"=="" set "STOCK_AI_WIKI_BODY_MAX_FETCH=10"

if "%STOCK_AI_WIKI_BODY_SKIP%"=="1" (
  echo [STOCK_AI_WIKI] STOCK_AI_WIKI_BODY_SKIP=1; skip fetch_article_body
) else (
  call :RUN_PY Stock-AI-Wiki\tools\fetch_article_body.py --article-archive-json "%ARCHIVE_JSON%" --max-fetch %STOCK_AI_WIKI_BODY_MAX_FETCH% --apply
  if errorlevel 1 (
    echo [WARN] fetch_article_body.py failed; continuing without article bodies
  )
)

call :RUN_PY Stock-AI-Wiki\tools\generate_coverage_notes.py --input "%COVERAGE_CSV%" --date "%STOCK_AI_WIKI_DATE%" --limit %STOCK_AI_WIKI_LIMIT% --probe-json "%PROBE_JSON%" --article-archive-json "%ARCHIVE_JSON%" --apply
if errorlevel 1 goto :FAIL

call :RUN_PY Stock-AI-Wiki\tools\update_wiki_links_from_article_seed.py --article-archive-json "%ARCHIVE_JSON%" --note-date "%STOCK_AI_WIKI_DATE%" --apply
if errorlevel 1 goto :FAIL

call :RUN_PY Stock-AI-Wiki\tools\build_progress_dashboard.py --apply
if errorlevel 1 goto :FAIL

echo [OK] Stock-AI-Wiki update finished
call :RELEASE_LOCK
popd
exit /b 0

:RUN_PY
echo [RUN] %*
"%PY%" %*
exit /b %ERRORLEVEL%

:RELEASE_LOCK
if defined LOCK_DIR if exist "%LOCK_DIR%\owner.txt" del /f /q "%LOCK_DIR%\owner.txt" >nul 2>nul
if defined LOCK_DIR if exist "%LOCK_DIR%" rmdir "%LOCK_DIR%" >nul 2>nul
exit /b 0

:FAIL
set "RC=%ERRORLEVEL%"
echo [FAILED] Stock-AI-Wiki update failed rc=%RC%
call :RELEASE_LOCK
popd
exit /b %RC%
