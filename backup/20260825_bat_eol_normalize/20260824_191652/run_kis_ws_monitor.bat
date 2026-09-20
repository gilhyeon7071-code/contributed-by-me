@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

set "PY="
if exist "%ROOT%_runtime\python312-embed\python.exe" set "PY=%ROOT%_runtime\python312-embed\python.exe"
if not defined PY if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
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
  exit /b 9009
)

if "%WS_MAX_CODES%"=="" set "WS_MAX_CODES=40"
if "%WS_CHANNELS%"=="" set "WS_CHANNELS=trade"
if "%WS_MOCK%"=="" set "WS_MOCK=auto"
if "%WS_DURATION_SEC%"=="" set "WS_DURATION_SEC=0"
if "%WS_MAX_SUBSCRIPTIONS%"=="" set "WS_MAX_SUBSCRIPTIONS=40"
if "%WS_NOTIFY_ON_ERROR%"=="" set "WS_NOTIFY_ON_ERROR=1"
if "%WS_ENABLE_HOGA%"=="" set "WS_CHANNELS=trade"

if exist "%ROOT%.secrets\kis_app_key_prod_2.txt" if exist "%ROOT%.secrets\kis_app_secret_prod_2.txt" if exist "%ROOT%.secrets\kis_account_no_prod_2.txt" (
  if "%KIS_APP_KEY_FILE_PROD%"=="" set "KIS_APP_KEY_FILE_PROD=%ROOT%.secrets\kis_app_key_prod_2.txt"
  if "%KIS_APP_SECRET_FILE_PROD%"=="" set "KIS_APP_SECRET_FILE_PROD=%ROOT%.secrets\kis_app_secret_prod_2.txt"
  if "%KIS_ACCOUNT_NO_FILE_PROD%"=="" set "KIS_ACCOUNT_NO_FILE_PROD=%ROOT%.secrets\kis_account_no_prod_2.txt"
  if "%KIS_TOKEN_CACHE_FILE%"=="" set "KIS_TOKEN_CACHE_FILE=%ROOT%2_Logs\kis_token_cache_prod_2.json"
)

set "ARGS=--max-codes %WS_MAX_CODES% --channels %WS_CHANNELS% --mock %WS_MOCK% --duration-sec %WS_DURATION_SEC% --max-subscriptions %WS_MAX_SUBSCRIPTIONS%"
if "%WS_NOTIFY_ON_ERROR%"=="1" set "ARGS=%ARGS% --notify-on-error"

echo [WS_MULTIPLEXER] PY=%PY%
echo [WS_MULTIPLEXER] ARGS=%ARGS%

"%PY%" tools\kis_ws_multiplexer.py %ARGS%
set "RC=%ERRORLEVEL%"

echo [WS] exit=%RC%
exit /b %RC%
