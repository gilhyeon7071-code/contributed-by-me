@echo off
setlocal EnableExtensions
set "ROOT=%~dp0"
pushd "%ROOT%" || exit /b 2

set "PY="
if exist "%ROOT%.venv\Scripts\python.exe" set "PY=%ROOT%.venv\Scripts\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY set "PY=python"

echo [RUN] tools\indicator_diag_and_recommend.py
%PY% tools\indicator_diag_and_recommend.py --lookback-days 120 --min-universe 2000 --horizons 1,2,5
set "RC=%ERRORLEVEL%"
if %RC% neq 0 echo [FAILED] indicator diag + recommend rc=%RC%
if %RC% equ 0 echo [OK] indicator diag + recommend done

popd
endlocal & exit /b %RC%
