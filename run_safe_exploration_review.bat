@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY="
if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not defined PY if exist "%~dp0.venv\Scripts\python.exe" set "PY=%~dp0.venv\Scripts\python.exe"
if not defined PY if exist "E:\1_Data\_runtime\python312-embed\python.exe" set "PY=E:\1_Data\_runtime\python312-embed\python.exe"
if not defined PY if exist "E:\vibe\buffett\.venv\Scripts\python.exe" set "PY=E:\vibe\buffett\.venv\Scripts\python.exe"
if not defined PY if exist "C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe" set "PY=C:\Users\jjtop\AppData\Local\Programs\Python\Python312\python.exe"
if not defined PY set "PY=python"

echo [SAFE_EXPLORATION] PY=%PY%
"%PY%" "%~dp0tools\safe_exploration_review.py" %*
set "RC=%ERRORLEVEL%"
echo [SAFE_EXPLORATION] exit=%RC%
exit /b %RC%
