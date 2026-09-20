@echo off
setlocal EnableExtensions

set "ROOT=E:\1_Data"
set "PY="
if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
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
