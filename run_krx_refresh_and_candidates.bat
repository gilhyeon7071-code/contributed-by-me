@echo off
setlocal EnableExtensions EnableDelayedExpansion

chcp 65001 >nul
cd /d %~dp0

set "PY="
if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
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

echo [1/2] Refresh KRX clean parquet (incremental to prev weekday)
"%PY%" krx_update_clean_incremental.py
if errorlevel 1 (
  echo [FAILED] krx_update_clean_incremental.py
  exit /b 1
)
echo.
echo [1.5/2] Refresh DART fundamental snapshot (optional)
set "DART_KEY_FILE=%~dp0_cache\dart_api_key.txt"
if "%DART_API_KEY%"=="" (
  if exist "%DART_KEY_FILE%" (
    echo [INFO] DART_API_KEY empty : using key file
    "%PY%" tools\build_dart_fundamental_snapshot.py --api-key-file "%DART_KEY_FILE%" --max-codes 300
    if errorlevel 1 (
      echo [WARN] DART refresh failed - continue with cached snapshot
    )
  ) else (
    echo [INFO] DART refresh skipped (no DART_API_KEY / dart_api_key.txt)
  )
) else (
  echo [INFO] DART_API_KEY detected : refresh snapshot
  "%PY%" tools\build_dart_fundamental_snapshot.py --max-codes 300
  if errorlevel 1 (
    echo [WARN] DART refresh failed - continue with cached snapshot
  )
)

echo.
echo [2/2] Regenerate candidates/meta (v41.1)
"%PY%" generate_candidates_v41_1.py
if errorlevel 1 (
  echo [FAILED] generate_candidates_v41_1.py
  exit /b 1
)

echo.
echo [OK] KRX clean ^& candidates refreshed
exit /b 0





