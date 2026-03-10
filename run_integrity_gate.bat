@echo off
setlocal EnableExtensions

set "ROOT=E:\1_Data"
set "PY="
if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\venv\Scripts\python.exe" set "PY=%ROOT%\venv\Scripts\python.exe"
if not defined PY if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
if not defined PY set "PY=python"

echo [IGATE] PY=%PY%
"%PY%" "%ROOT%\tools\integrity_gate_enforce.py" %*
set "RC=%ERRORLEVEL%"
echo [IGATE] exit=%RC%
exit /b %RC%
