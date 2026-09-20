@echo off
setlocal EnableExtensions

set "ROOT=E:\1_Data"
set "PY="
REM [2026-08-25] 공식 embed 를 1순위로. 이 파일은 embed 가 없어서 후보가
REM   vibe venv -> %ROOT%\venv -> %ROOT%\.venv(린트 전용, pandas 없음) 순이었다.
REM   vibe venv 가 사라지면 08-07 과 같은 shadowing 으로 깨진다.
if exist "%ROOT%\_runtime\python312-embed\python.exe" set "PY=%ROOT%\_runtime\python312-embed\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\venv\Scripts\python.exe" set "PY=%ROOT%\venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
if not defined PY set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"

call "%PY%" -V >nul 2>&1
if errorlevel 1 set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
call "%PY%" -V >nul 2>&1
if errorlevel 1 set "PY=python"

echo [NIGHTLY_INTEGRITY] PY=%PY%
"%PY%" "%ROOT%\tools\nightly_data_integrity_check.py" %*
set "RC=%ERRORLEVEL%"
echo [NIGHTLY_INTEGRITY] exit=%RC%
exit /b %RC%
