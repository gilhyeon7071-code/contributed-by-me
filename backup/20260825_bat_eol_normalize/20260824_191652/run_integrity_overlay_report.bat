@echo off
setlocal EnableExtensions

set "ROOT=E:\1_Data"
set "PY="
if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\venv\Scripts\python.exe" set "PY=%ROOT%\venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
if not defined PY set "PY=python"

echo [INTEGRITY] PY=%PY%
"%PY%" "%ROOT%\tools\build_integrity_overlay_report.py" %*
set "RC=%ERRORLEVEL%"
echo [INTEGRITY] exit=%RC%
exit /b %RC%
